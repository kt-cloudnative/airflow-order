from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG632B',WORKFLOW_ID='0a7691c88fd0478e8cd9e214fbb012bb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG632B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 6, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0a7691c88fd0478e8cd9e214fbb012bb')

    csng632bJob_vol = []
    csng632bJob_volMnt = []
    csng632bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng632bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng632bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng632bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng632bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng632bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '465ea7496e22432fbc25db53aaa9479b',
        'volumes': csng632bJob_vol,
        'volume_mounts': csng632bJob_volMnt,
        'env_from':csng632bJob_env,
        'task_id':'csng632bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.56',
        'arguments':["--job.name=csng632bJob", "requestDate=${YYYYMMDDHHMISSSSS}"
, "endTranDate=${YYYYMM}","pgmNm=csng632b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0a7691c88fd0478e8cd9e214fbb012bb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng632bJob,
        Complete
    ]) 

    # authCheck >> csng632bJob >> Complete
    workflow








