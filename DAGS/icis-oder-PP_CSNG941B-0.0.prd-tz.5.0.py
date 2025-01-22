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
                , WORKFLOW_NAME='PP_CSNG941B',WORKFLOW_ID='bd08703ec81048b39076838299d844ae', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG941B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 28, 19, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bd08703ec81048b39076838299d844ae')

    csng941bJob_vol = []
    csng941bJob_volMnt = []
    csng941bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng941bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng941bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng941bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng941bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng941bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f1d24a94204343608e0d8a61992fc455',
        'volumes': csng941bJob_vol,
        'volume_mounts': csng941bJob_volMnt,
        'env_from':csng941bJob_env,
        'task_id':'csng941bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng941bJob", "endTranDate=20240603", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bd08703ec81048b39076838299d844ae')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng941bJob,
        Complete
    ]) 

    # authCheck >> csng941bJob >> Complete
    workflow








