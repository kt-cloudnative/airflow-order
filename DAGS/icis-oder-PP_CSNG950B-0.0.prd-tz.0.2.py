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
                , WORKFLOW_NAME='PP_CSNG950B',WORKFLOW_ID='bdf15c493a894d4999870dc4e822f6dc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG950B-0.0.prd-tz.0.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 11, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bdf15c493a894d4999870dc4e822f6dc')

    csng950bJob_vol = []
    csng950bJob_volMnt = []
    csng950bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng950bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng950bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng950bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng950bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng950bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c0d0cc9f3616493297bafb85c7c9e797',
        'volumes': csng950bJob_vol,
        'volume_mounts': csng950bJob_volMnt,
        'env_from':csng950bJob_env,
        'task_id':'csng950bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng950bJob","workDate=20240430", "requestDate="+str(datetime.now().strftime("%Y%m%d%H%M%S"))],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bdf15c493a894d4999870dc4e822f6dc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng950bJob,
        Complete
    ]) 

    # authCheck >> csng950bJob >> Complete
    workflow








