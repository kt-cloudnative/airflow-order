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
                , WORKFLOW_NAME='PP_CSNG590B',WORKFLOW_ID='3cc4b1f6bdb4483dad4c65315b1863db', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG590B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3cc4b1f6bdb4483dad4c65315b1863db')

    csng590bJob_vol = []
    csng590bJob_volMnt = []
    csng590bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng590bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng590bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng590bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng590bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng590bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng590bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng590bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '64e767642f0e48e4bd9c683785b2aa01',
        'volumes': csng590bJob_vol,
        'volume_mounts': csng590bJob_volMnt,
        'env_from':csng590bJob_env,
        'task_id':'csng590bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng590bJob", "endTranDate=20241129",  "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3cc4b1f6bdb4483dad4c65315b1863db')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng590bJob,
        Complete
    ]) 

    # authCheck >> csng590bJob >> Complete
    workflow








