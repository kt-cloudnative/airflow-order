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
                , WORKFLOW_NAME='PP_CSND222B',WORKFLOW_ID='d3865ce63b524c629055a1dbf1111ef5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND222B-0.0.prd-tz.2.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 21, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d3865ce63b524c629055a1dbf1111ef5')

    csnd222bJob_vol = []
    csnd222bJob_volMnt = []
    csnd222bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd222bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd222bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd222bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd222bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd222bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'df2232f05fd9470392a9080735743e2d',
        'volumes': csnd222bJob_vol,
        'volume_mounts': csnd222bJob_volMnt,
        'env_from':csnd222bJob_env,
        'task_id':'csnd222bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csnd222bJob", "endTranDate=20240528", "pgmNm=csnd222b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d3865ce63b524c629055a1dbf1111ef5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd222bJob,
        Complete
    ]) 

    # authCheck >> csnd222bJob >> Complete
    workflow








